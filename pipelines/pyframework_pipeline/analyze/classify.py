"""C2 classify subflow: normalized records -> classified records (CPython 14 cat).

Path-in/path-out: reads a perf_records.csv (uncategorized), classifies each
record's symbol/shared_object against the CPython category rules, and writes a
classified_records.csv with category_top/category_sub/category_reason filled.

The classification rules are CPython domain knowledge (fixed, framework-
agnostic); they ship with the package. A custom rules file may be supplied.
"""
from __future__ import annotations

from pathlib import Path

from .perf_analysis_common import (
    NORMALIZED_FIELDS,
    Rule,
    classify_record,
    load_rules,
    read_csv_rows,
    write_csv_rows,
)

_BUILTIN_RULES = Path(__file__).resolve().parent / "cpython_category_rules.json"


class CategoryClassifier:
    """Classify a record's symbol/shared_object into CPython categories.

    Rules are CPython domain knowledge (fixed, framework-agnostic). The default
    rules ship with the package; a custom rules file may be supplied.
    """

    def __init__(self, rules_path: Path | None = None) -> None:
        if rules_path is None:
            rules_path = _BUILTIN_RULES
        self.rules: list[Rule] = load_rules(rules_path)

    def classify(self, symbol: str, shared_object: str) -> tuple[str, str, str]:
        """Return (category_top, category_sub, category_reason) for one record."""
        return classify_record(
            {"symbol": symbol or "", "shared_object": shared_object or ""},
            self.rules,
        )


def classify(
    *,
    input_path: Path = Path("perf_records.csv"),
    output_path: Path = Path("classified_records.csv"),
    rules_path: Path | None = None,
) -> Path:
    """Classify a records CSV in place (path-in/path-out).

    Reads ``input_path`` (uncategorized PerfRecord CSV), writes ``output_path``
    with the category columns filled. Returns the output path.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    classifier = CategoryClassifier(rules_path=rules_path)

    rows = read_csv_rows(input_path)
    for row in rows:
        top, sub, reason = classifier.classify(row.get("symbol", ""), row.get("shared_object", ""))
        row["category_top"] = top
        row["category_sub"] = sub
        row["category_reason"] = reason

    write_csv_rows(output_path, NORMALIZED_FIELDS, rows)
    return output_path


__all__ = ["CategoryClassifier", "classify"]
